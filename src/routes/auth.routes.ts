import { Router, Request, Response } from 'express';
import { AuthService } from '../services/auth.service';
import { body, validationResult } from 'express-validator';
import { auth } from '../middleware/auth.middleware';
import { userRepository } from '../repositories/user.repository';
import { AuthRequest } from '../types/express';

const router = Router();
const authService = new AuthService();

router.post('/register', 
  [
    body('username').isString().notEmpty(),
    body('email').isEmail(),
    body('password').isLength({ min: 6 })
  ],
  async (req: Request, res: Response) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    try {
      const { user, token } = await authService.register(req.body);
      res.status(201).json({ user, token });
    } catch (error) {
      res.status(400).json({ error: (error as Error).message });
    }
  }
);

router.post('/token', 
  [
    body('email').isEmail(),
    body('password').exists()
  ],
  async (req: Request, res: Response) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    try {
      const { email, password } = req.body;
      const { user, token } = await authService.login(email, password);
      res.json({ user, token });
    } catch (error) {
      res.status(401).json({ error: (error as Error).message });
    }
  }
);

// Protected route that requires authentication
router.get('/login', auth, async (req: AuthRequest, res: Response) => {
  try {
    const user = await userRepository.findById(req.user.id);
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }
    const { password, ...userWithoutPassword } = user;
    res.json(userWithoutPassword);
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

export default router; 